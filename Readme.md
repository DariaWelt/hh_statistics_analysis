## Dependencies
- dotnet 6.0
- mongodb 6.0.1
- mongosh 1.6.0

## Run
1. start MongoDB on port 9200
  ```
  mongod --dbpath ./database --port 9200 --fork --logpath ./mongod.log
  ```
2. load database dump if it exists
  ```
  mongorestore -db --port 9200 ./backups/<DATE>
  ```
3. create database "vacations" inside mongoDB
```
mongosh "mongodb://localhost:9200"
use hh_vacations
exit
```
3. compile and start LazyExtractor
```
cd DataExtractor
dotnet build LazyExtractorService --output ./build_output    
dotnet ./build_output/LazyExtractorService.dll --LazyExtractor:workers=5
```
4. enjoy with results
```
mongosh "mongodb://localhost:9200"
use hh_vacations
db.hh_vacancies_RAW.find().pretty()
exit
```
5. save database and terminate
```
mongodump --port=9200 --gzip --out ./backups/$(date +"%Y-%m-%d")
mongosh "mongodb://localhost:9200"
use admin;
db.shutdownServer();
exit
```