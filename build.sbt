/**
* SBT main configuration
*
* 
*/
lazy val commonSettings = Seq( 
  version := "0.1.0",
  scalaVersion := "2.10.4"   
)

//Assembly plugin settings
lazy val assemblySettings = Seq(
)



//Repositories
lazy val repositories = Seq(
  //resolvers += "Central Maven" at "http://central.maven.org/maven2/"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(assemblySettings: _*).
  settings(repositories: _*).
  settings(
    name := "IncreaseFinder",
     
    libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.3"
	
  )
  
  
  