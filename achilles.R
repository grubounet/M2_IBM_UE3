Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "c:/temp/jdbcDrivers")


library(DatabaseConnector)
library(Achilles)


connectionDetails <- createConnectionDetails(dbms = "postgresql", 
                                             server = "127.0.0.1/omop_projet", 
                                             user = "postgres", 
                                             password = "mypass")

achilles(connectionDetails = connectionDetails,
         cdmDatabaseSchema = "public",
         resultsDatabaseSchema = "public_results",
         vocabDatabaseSchema = "public",
         sourceName = "omop_projet",
         outputFolder = "achilles_output",
         createTable = TRUE,
         smallCellCount = 5,
         cdmVersion = "5.4",
         createIndices = TRUE,
         numThreads = 1,
         tempAchillesPrefix = "tmpach",
         dropScratchTables = FALSE,
         sqlOnly = FALSE,
         verboseMode = TRUE,
         optimizeAtlasCache = TRUE,
         defaultAnalysesOnly = TRUE,
         updateGivenAnalysesOnly = FALSE,
         excludeAnalysisIds = FALSE,
         sqlDialect = "postgresql"
         
         )

