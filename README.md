#Apache Beam Example using FlinkRunner

##Code
First of all, we need to compile the code. You can use the *java* docker image for that purpose:
- docker run -ti -v $(pwd)/examples/:/src java bash
- (once in the container)
- cd /src
- mvn clean package

Now, the *examples/target* folder will contain a *jar* file with the ar.uba.fi.beam.WordCount in a lean and bundled version. The bundled version will be required within the Runners in order to have all the java dependencies in place to instanciate the job.

##Flink runner
We'll need to start a Flink server including Job and Task managers. The Job manager will provide a Flink Dashboard which will accept the *jar* bundle to launch the Job. The Task managers are slaves which allows horizontal scalability for job running.
- docker-compose up
- (or)
- docker-compose up scale taskmanager=3

Then, we can upload the Job code:
- Go to http://localhost:8081
- Click on 'Submit new Job'
- Click on 'Add New'
- Choose the file *examples/target/taller3-flink-examples-bundled-0.1.jar*
- Click on 'Upload'

And, lauch it:
- Mark the checkbox next to *taller3-flink-examples-bundled-0.1.jar* in the uploaded Jars' table.
- Define the entry class: *ar.uba.fi.beam.WordCount*
- Define the program arguments: *--runner=FlinkRunner*
- Click on 'Submit'


