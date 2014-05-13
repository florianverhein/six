uniform.project("six", "au.com.cba.omnia.six")

libraryDependencies :=
  depend.hadoop() ++
  Seq(
    "com.cba.omnia"            %% "edge"                            % "0.3.1-20131030152821"      jar(),
    "com.cba.omnia"            %% "core-features"                   % "0.10.0-20131220103427",
    "com.cba.omnia"            %% "utils"                           % "0.9.1-20131120112638"
  )

resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"

uniformAssemblySettings
