val ZioVersion     = "1.0.0-RC10-1"
val Specs2Version  = "4.6.0"
val CatsVersion    = "2.0.0-M4"
val CatsEffVersion = "2.0.0-M3"

resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.sonatypeRepo("snapshots")

lazy val root = (project in file("."))
  .settings(
    organization := "Clover Group",
    name := "tsp-doobie",
    version := "0.0.1",
    scalaVersion := "2.12.8",
    maxErrors := 3,
    libraryDependencies ++= Seq(
      "dev.zio"       %% "zio"         % ZioVersion,
      "org.typelevel" %% "cats-core"   % CatsVersion,
      "org.typelevel" %% "cats-effect" % CatsEffVersion,
      "org.specs2"    %% "specs2-core" % Specs2Version % "test",
      "com.dimafeng" %% "testcontainers-scala" % "0.27.0",
      "org.testcontainers" % "postgresql" % "1.11.3",
      "org.testcontainers" % "clickhouse" % "1.11.3" % "test",
      "org.slf4j" % "slf4j-simple" % "1.7.21", // logger implementation"org.tpolecat" %% "doobie-core"      % "0.7.0",
      "org.postgresql" % "postgresql" % "9.4-1206-jdbc42",
      "org.tpolecat" %% "doobie-core" % "0.7.0",
      "org.tpolecat" %% "doobie-postgres" % "0.7.0",
      "org.tpolecat" %% "doobie-hikari"    % "0.7.0",
      "dev.zio" %% "zio-interop-cats" % "1.0.0-RC8-9",
      "org.scalatest" %% "scalatest" % "3.0.8" % "test"
    )
  )

// Refine scalac params from tpolecat
scalacOptions --= Seq(
  "-Xfatal-warnings"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("chk", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
