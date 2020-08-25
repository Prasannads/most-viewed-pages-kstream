# Most viewed pages

## Logic:
- Joins the messages in these two topics on the user id field
- Uses a 1 minute hopping window with 10 second advances to compute the 10 most viewed
pages by viewtime for every value of gender
- Once per minute produces a message into the top_pages topic that contains the gender,
page id, sum of view time in the latest window and distinct count of user ids in the latest
window

## SQL Equivalent (not tested)

```
SELECT * 
FROM   (SELECT gender, 
               pageid, 
               COUNT(DISTINCT pageviews.userid) AS distinctusers, 
               SUM(viewtime)                    AS sumviewtime, 
               ROW_NUMBER() 
                 OVER ( 
                   partition BY gender, pageid 
                   ORDER BY sumviewtime DESC) AS rank 
        FROM   users 
               JOIN pageviews 
                 ON users.userid = pageviews.userid 
        GROUP  BY 1, 2 
        ORDER  BY 1, 2) 
WHERE  rank <= 10 
```

## How to Run

1. Build the project

`avro:generate` (optional)
`clean;assembly` (from sbt-shell) or `sbt "clean;assembly"` (from terminal)

2. Run docker-compose

```bash
cd docker-local-setup

docker-compose up -d
```

Running docker compose will 
    - add schemas to schema Registry.
    - create topics.
    - start the connectors.
    - start the kstream application.
    
Once the containers are up and running, go to `http://localhost:9000/` to view the messages (chose Avro as message format) in the 
output topic `topppages`.
Note: `most-viewed-pages-kstream` container will wait for other services to come up. Because, technically as per docker,
containers are up, but the services might take some time. Check `./start-sh` for more info.
    
## Important Folders/Files.

- ./docker-local-setup/connector-configuration -> Connector configurations for `users` and `pageviews`.
- ./docker-local-setup/schemas -> Schemas of source and destination topics.
- ./docker-local-setup/create-schemas-topics-and-connectors.sh -> Shell script to create schemas, topics and connectors.
- ./start-sh -> Shell script. Starting point for Kstream service.
    

## Notes to the reader

1. Unfortunately, I have to mark the tests as `pending` as the tests are not properly working with custom 
serializers/deserializers for POJO classes. Also, due to time constraints(packed and extended work hours), 
I could not spend much time to fix them. I would love to fix them for sure and write more unit tests.

2. I use [kafdrop](https://github.com/obsidiandynamics/kafdrop) for easy viewing of messages, but messages can be viewed 
from control center too.
