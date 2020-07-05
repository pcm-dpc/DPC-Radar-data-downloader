# Radar DPC Downloader - Versione Java

## Requisiti

Java 1.8 o superiore

## Costruzione dell'applicazione

> git clone ....&& cd ...
>
> mvn clean install

nel file src/main/resources/application.properties modificare la lista dei prodotti che si vuole scaricare e il path in cui verranno scritti i dati scaricati

> productToDownload  = VMI,SRI,SRT1,SRT3,SRT6,SRT12,SRT24,TEMP,HRD
>
> defaultSavePath = /home/user/DATI_RADAR/
 
## Avvio del client
 
 > mvn spring-boot:run

## Utilizzo della release

> export SPRING_CONFIG_LOCATION=file:///home/user/application.properties
>
> java -jar download-client-1.0.jar


## Documentazione di riferimento

* [Official Radar DPC Documentation](https://dpc-radar.readthedocs.io/it/latest/) 
* [Official Apache Maven documentation](https://maven.apache.org/guides/index.html)
* [Spring Boot Maven Plugin Reference Guide](https://docs.spring.io/spring-boot/docs/2.3.1.RELEASE/maven-plugin/reference/html/)
* [WebSocket](https://docs.spring.io/spring-boot/docs/2.3.1.RELEASE/reference/htmlsingle/#boot-features-websockets)
