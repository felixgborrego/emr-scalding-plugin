sbtPlugin := true

name := "emr-scalding-plugin"

organization := "com.gilt"

libraryDependencies += "com.gilt" %% "lib-emr-manager" % "0.0.1"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")
