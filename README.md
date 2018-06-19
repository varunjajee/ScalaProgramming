

- How to build sbt/scala project

Running the project
]$ sbt run

Compiling the project
]$ sbt compile


- Continous run the project
]$ sbt
sbt:ScalaProgramming> ~ run
1. Waiting for source to changes...

>>> and will start running the Main.scala as an when either of the files changes in the proejct.

- Continous compilation
]$ sbt
sbt:ScalaProgramming> ~ compile
1. Waiting for source to changes...

>>> and will start compiling the Main.scala as an when either of the files changes in the proejct.


- Run as jar
]$ sbt pacakge

]$ scala <jar file path> | scala -classpath <jar file path>


