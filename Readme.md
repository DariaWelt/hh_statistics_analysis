# HH vacancies analyzer
App for statistical analysis of description of vacancies with GUI and
horizontal scalability by using Spark for computation and Kafka for services communication

### Analytical tasks:
1. Share of vacancies requiring this technology
2. The Correlation of technologies by vacancies
3. Mean salary by technology
4. The impact of tech knowledge on salary by technology

## Deploy with docker
1. If you have database backup put it to project folder with directory name `mongodb_data`
2. Build Images and run containers
   ```
   docker compose build && docker compose up
   ```
   
3. Go to [127.0.0.1/8050](127.0.0.1/8050) to use user interface
4. Click `"extract"` button to run month extractor service
5. Type technologies that you want to process by `;`  and click `"process"` button to run handler service 
and get statistics

To inspect database row data that was been collected run sequentially:
```
docker exec -it hh_statistics_analysis-db-1 bash
mongo
use hh_vacancies
db.hh_vacancies_RAW.count()
db.hh_vacancies_RAW.find({})
exit
^p
^q
```