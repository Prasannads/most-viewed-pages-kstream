package com.data.mostviewedpages

import com.data.mostviewedpages.configuration.Configuration
import com.data.mostviewedpages.serdes.{KafkaBytesDeserializer, KafkaBytesSerializer}
import com.data.mostviewedpages.utils.SerdeUtils

import java.time.Duration
import java.util
import com.data.mostviewedpages.serdes.KafkaBytesSerializer
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import ksql._
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{GlobalKTable, TimeWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Suppressed
import org.apache.kafka.streams.scala.kstream.Suppressed.BufferConfig

// POJO classes
case class GenderPageId(gender: String, pageId: String)

case class PageViewsWithUser(GenderPageId: GenderPageId, userId: String, viewTime: Long)

case class PageViewsWithDistinctUsers(gender: String,
                                      pageId: String,
                                      distinctUsers: Long,
                                      sumOfViewTime: Long)

case class PageViewsWithUserCollection(windowSet: Array[PageViewsWithDistinctUsers])

object TopPageViews extends App with AppMain with LazyLogging {

  // Implicits for Serdes
  implicit val usersSerde: SpecificAvroSerde[users]         = SerdeUtils.configValueSerde[users]
  implicit val pageViewsSerde: SpecificAvroSerde[pageviews] = SerdeUtils.configValueSerde[pageviews]
  implicit val topPagesSerde: SpecificAvroSerde[toppages]   = SerdeUtils.configValueSerde[toppages]
  implicit val pageViewsWithDistinctUsersSerde: SpecificAvroSerde[pageviewswithdistinctusers] =
    SerdeUtils.configValueSerde[pageviewswithdistinctusers]
  implicit val pageViewsWithUserSerde: Serde[PageViewsWithUser] =
    Serdes.serdeFrom(new KafkaBytesSerializer[PageViewsWithUser],
                     new KafkaBytesDeserializer[PageViewsWithUser])
  implicit val pageViewsWithUserCollectionSerde: Serde[PageViewsWithUserCollection] =
    Serdes.serdeFrom(new KafkaBytesSerializer[PageViewsWithUserCollection],
                     new KafkaBytesDeserializer[PageViewsWithUserCollection])
  implicit val genderPageIdSerde: Serde[GenderPageId] =
    Serdes.serdeFrom(new KafkaBytesSerializer[GenderPageId],
                     new KafkaBytesDeserializer[GenderPageId])
  implicit val pageViewWithDistinctUsersSerde: Serde[PageViewsWithDistinctUsers] =
    Serdes.serdeFrom(new KafkaBytesSerializer[PageViewsWithDistinctUsers],
                     new KafkaBytesDeserializer[PageViewsWithDistinctUsers])

  // Create Hopping Windows with an Advance.
  val windowSizeMs: Duration  = Duration.ofMinutes(1)
  val advanceSizeMs: Duration = Duration.ofSeconds(10)
  val hoppingWindow           = TimeWindows.of(windowSizeMs).advanceBy(advanceSizeMs)

  // Build StreamBuilder.
  val builder: StreamsBuilder = initTopology

  // Create a Global KTable for Users.
  val userGlobalKtable: GlobalKTable[String, users] =
    buildGlobalKTable[String, users](builder, Configuration.app.usersTopic)

  // Run Transformations.
  transformations(
    builder,
    userGlobalKtable,
    Configuration.app.pageViewTopic,
    Configuration.app.topPagesTopic
  )

  // Create a Topology
  val topology: Topology = buildTopology(builder)
  logger.info(s"Topology for this application is ${topology.describe()}")

  // Start Streaming
  stream(topology, Configuration.app.pageViewsAppId)

  private def transformations(builder: StreamsBuilder,
                              userGlobalKtable: GlobalKTable[String, users],
                              pageViewTopic: String,
                              topPagesTopic: String): Unit = {

    val pageViewsStream = builder
      .stream[String, pageviews](pageViewTopic)
      .selectKey((_, v) => v.getUserid.toString)

    /**
      * Join Users Global KTable with Pageviews.
      */
    val joinedPageViewStream =
      pageViewsStream
        .selectKey((_, v) => v.getUserid.toString)
        .join(userGlobalKtable)(
          (key, _) => key,
          (pageviews, users) => {
            PageViewsWithUser(GenderPageId(users.getGender.toString, pageviews.getPageid.toString),
                              users.getUserid.toString,
                              pageviews.getViewtime)
          }
        )

    /**
      * In a window of 1 min with 10 seconds advance - Group the events on Gender and Pageid to get
      * the sum of viewtime and distinct users in the group.
      */
    val topPagesStream = joinedPageViewStream
      .selectKey((_, v) => GenderPageId(v.GenderPageId.gender, v.GenderPageId.pageId))
      .groupByKey
      .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(10)))
      .aggregate[pageviewswithdistinctusers](initializeViewTime)((_, v, agg) => {
        val distinctUsers = scala.collection.mutable.Set[String]()
        viewTimeAggregator(v, agg, distinctUsers)
      })
      .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
      .toStream
      .selectKey((k, _) => k.key())
      .groupByKey
      .aggregate[pageviewswithdistinctusers](initializeViewTime)((k, v, agg) => {
        new pageviewswithdistinctusers(k.gender,
                                       k.pageId,
                                       Math.max(v.getDistinctusers, agg.getDistinctusers),
                                       Math.max(v.getViewtime, agg.getViewtime))
      })
      .toStream

    /**
      * Fetch top 10 page views based on the summed viewtime per gender and page id
      * from the 1 min windowed group.
      */
    topPagesStream
      .selectKey((k, _) => k.gender)
      .mapValues(
        (_, v) =>
          PageViewsWithDistinctUsers(v.getGender.toString,
                                     v.getPageid.toString,
                                     v.getDistinctusers,
                                     v.getViewtime))
      .groupByKey
      .aggregate[PageViewsWithUserCollection](initializeTopViewsCollection)((_, v, agg) =>
        top10Aggregator(v, agg))
      .toStream
      .flatMapValues(pageviews => pageviews.windowSet)
      .filter((_, v) => v != null)
      .mapValues((_, v) => new toppages(v.gender, v.pageId, v.distinctUsers, v.sumOfViewTime))
      .to(topPagesTopic)
  }

  private def initializeViewTime: pageviewswithdistinctusers = {
    new pageviewswithdistinctusers("", "", 0L, 0L)
  }

  private def initializeTopViewsCollection: PageViewsWithUserCollection = {
    val pageUserViews = Array.ofDim[PageViewsWithDistinctUsers](11) // Allow only top 10 in a window.
    PageViewsWithUserCollection(pageUserViews)
  }

  /**
    * Aggregator to sum the viewtime and calculate distinct users in a hopping window.
    * @param newValue new value.
    * @param aggValue aggregator.
    * @return Sum of view time and distinct users as pagewithdistinctusers.
    */
  private def viewTimeAggregator(
      newValue: PageViewsWithUser,
      aggValue: pageviewswithdistinctusers,
      users: scala.collection.mutable.Set[String]): pageviewswithdistinctusers = {
    users += newValue.userId
    val sumViewTime   = aggValue.getViewtime + newValue.viewTime
    val distinctUsers = aggValue.getDistinctusers + users.size
    new pageviewswithdistinctusers(newValue.GenderPageId.gender,
                                   newValue.GenderPageId.pageId,
                                   distinctUsers.toLong,
                                   sumViewTime.toLong)
  }

  /**
    * Aggregator with custom Sort comparator on viewtime to get the top 10.
    * @param newValue new value.
    * @param aggValue aggregator.
    * @return Top 10 viewed pages as PageViewsWithUserCollection.
    */
  private def top10Aggregator(
      newValue: PageViewsWithDistinctUsers,
      aggValue: PageViewsWithUserCollection): PageViewsWithUserCollection = {
    aggValue.windowSet(10) = newValue
    util.Arrays.sort(
      aggValue.windowSet,
      (a: PageViewsWithDistinctUsers, b: PageViewsWithDistinctUsers) => {
        var aValue = 0L
        var bValue = 0L
        // Initial value of every batch will be nulls
        aValue = if (a == null) 1L else a.sumOfViewTime
        bValue = if (b == null) -1L else b.sumOfViewTime
        java.lang.Long.compare(bValue, aValue)
      }
    )
    aggValue.windowSet(10) = null
    aggValue
  }

}
