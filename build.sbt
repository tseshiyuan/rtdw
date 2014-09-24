import AssemblyKeys._ // put this at the top of the file

assemblySettings

organization := "com.saggezza.lubeinsightsplatform"

name := "lubeinsightsplatform"

//scalaVersion := "2.10.4"

//version := "1.0-RELEASE"
version := "1.0-SNAPSHOT"

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= Seq( "repo1" at "http://repo1.maven.org/maven2/" )

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter {(x)=> x.data.getName.endsWith(".SF") || x.data.getName.endsWith(".RSA") || x.data.getName.endsWith(".DSA")}
}

mergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) => MergeStrategy.rename
//      case ("meta-inf.*\\.sf$" :: Nil) => MergeStrategy.discard
//      case ("meta-inf.*\\.rsa$" :: Nil) => MergeStrategy.discard
//      case ("meta-inf.*\\.dsa$" :: Nil) => MergeStrategy.discard
      case file @ (_ :: Nil) if file.head.startsWith("license") => MergeStrategy.discard
      case file @ (_ :: Nil) if file.last.endsWith("sf") => MergeStrategy.discard
      case file @ (_ :: Nil) if file.last.endsWith("rsa") => MergeStrategy.discard
      case file @ (_ :: Nil) if file.last.endsWith("dsa") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}


libraryDependencies ++= Seq(
"com.101tec" % "zkclient" % "0.4" exclude("org.apache.zookeeper", "zookeeper"),
"org.apache.spark" % "spark-core_2.10" % "1.0.2" exclude("org.apache.zookeeper", "zookeeper"),
"org.eclipse.jetty.aggregate" % "jetty-all-server" % "8.1.14.v20131031",
"org.hamcrest" % "hamcrest-all" % "1.3" % "test",
"junit" % "junit" % "4.4" % "test",
"com.yammer.metrics" % "metrics-core" % "2.2.0",
"net.sf.jopt-simple" % "jopt-simple" % "3.0-rc2",
"org.apache.zookeeper" % "zookeeper" % "3.3.1"
  /*excludeAll(
  ExclusionRule(organization = "org.apache.zookeeper") )*/
)

// assembly publisher
artifact in (Compile, assembly) ~= { art =>
  art.copy(`classifier` = Some("assembly"))
}

//addArtifact(artifact in (Compile, assembly), assembly)

// disable using the Scala version in output paths and artifacts
crossPaths := false

//publishTo := Some(Resolver.file("file",  new File( Path.userHome+"/.ivy2/local/" )) )

publishMavenStyle := true

//credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
//credentials += Credentials("Sonatype Nexus Repository Manager", "10.196.32.21", "me","mypassword")
