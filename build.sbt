lazy val root = (project in file("."))
  .settings(
    Seq(
      organization := "com.scylladb",
      name := "migrator-dynamo-mutator",
      version := "0.0.1",
      scalaVersion := "2.13.3",
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio" % "1.0.0",
        "dev.zio" %% "zio-streams" % "1.0.0",
        "dev.zio" %% "zio-test" % "1.0.0" % Test
      )
    )
  )
