## PREREQUISITES

- Installed Spark in Docker using image "docker pull apache/spark"
- Since it did not have necessary libraries such as pyspark, requests, and pygeohash, I customized a docker image.
To do that, a Dockerfile was created in a local directory, where content looks like this:
                        # Using the official Apache Spark image as the base image
                        FROM apache/spark:latest
                        # Setting the working directory inside the container
                        WORKDIR /opt/spark
                        # Switching to root user to install dependencies
                        USER root
                        RUN pip install --no-cache-dir pyspark requests pygeohash
                        # Switching back to the original user
                        USER spark
                        # Setting the environment variables for Spark
                        ENV SPARK_HOME=/opt/spark
                        ENV PATH=$SPARK_HOME/bin:$PATH
                        # Expose Spark's default port
                        EXPOSE 4040
                        # Start the Spark shell
                        CMD ["/opt/spark/bin/spark-submit"]
- Next I built this image: "docker build -t my_spark_image1 ."
- Then I combined the data from restaurants and three weather folder (For ease of calculation I took one dataset from each month, namely: 9_August_1-4, 17_September_1-5, 9_August_1-4) into one folder in the directory: "C:\Users\yeras\Downloads\task" (contains restaurant_csv folder and weather folder. Weather folder contains data of all three folders)
- Then I run a container "docker run -it -v C:\Users\yeras\Downloads\task:/data my_spark_image1 bash"

## TASK
- Below are the steps of the main task:
1. Checked for incorrect (null) latitude and longitude values in the restaurant dataset.
2. Used the "OpenCage Geocoding API" to map the latitude and longitude.
3. Generated a 4-character "geohash" for each restaurant.
4. Left joined the enriched restaurant data with the weather dataset using the 4-character geohash.
5. Stored the enriched data in Parquet format on the local file system.


## Files in the repository
- `yerassyl_script.py`: The main script to run the ETL job.
- `yerassyl_read_output.py`: Script to read and check the results.
- `tests.py`: Unit tests for the ETL job functionality.
- "restaurant_csv" folder contains datasets for restaurants
- "weather" folder contains datasets for weather
- "output/enriched_data" contains result files

## How to run the ETL job
- Once docker container is up and running I used "python3 /data/yerassyl_script.py" to run the main script. 
- When completed, to check the results used "python3 /data/yerassyl_read_output.py"

## Testing
- To test the ETL job, run the unit tests using the following command: "python3 /data/tests.py"
