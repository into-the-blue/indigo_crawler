docker-compose -f ./chromeSelenium/docker-compose.yml up -d
docker-compose -f ./urlCrawler/docker-compose.yml up -d
docker-compose -f ./detailCrawler/docker-compose.yml up -d
docker-compose -f ./dataValidator/docker-compose.yml up -d