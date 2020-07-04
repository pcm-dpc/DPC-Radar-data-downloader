# Radar DPC Data Downloader - Java Version

## Requirements

Java 1.8 or higher

## Build the app

> git clone ....&& cd ...
>
> mvn clean install

modify src/main/resources/application.properties the products list and the default save path

> productToDownload  = VMI,SRI,SRT1,SRT3,SRT6,SRT12,SRT24,TEMP,HRD
>
> defaultSavePath = /home/user/DATI_RADAR/
 
## Run the client
 
 > mvn spring-boot:run

## Use the release

> export SPRING_CONFIG_LOCATION=file:///home/user/application.properties
>
> java -jar download-client-1.0.jar


## Reference Documentation

* [Official Apache Maven documentation](https://maven.apache.org/guides/index.html)
* [Spring Boot Maven Plugin Reference Guide](https://docs.spring.io/spring-boot/docs/2.3.1.RELEASE/maven-plugin/reference/html/)
* [WebSocket](https://docs.spring.io/spring-boot/docs/2.3.1.RELEASE/reference/htmlsingle/#boot-features-websockets)
