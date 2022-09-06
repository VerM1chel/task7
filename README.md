# task7
Alert project

In DataPreparator class data is prepared (renaming columns, selecting only relevant data, setting the required date format, and grouping by bundle_id). In MinuteHandler and HourHandler classes warnings are displayed by minutes and hours, respectively. Each of the classes has 2 alert methods: batched_alert prints all minutes/hours with more than 10 warnings and streaming_alert displays information on the last minute/hour counting from the present time, which is suitable for streaming data processing.
To run the program, download this directory and enter ### docker run alert_project ### in the console
