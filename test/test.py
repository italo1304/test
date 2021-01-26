from pyspark.sql import SparkSession, Row, functions

def loadMovieNames():
    movieNames = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Testing").getOrCreate()

    movieNames = loadMovieNames()

    # lines = spark.sparkContext.textFile("hdfs://localhost:9000/user/vagrant/u.data")
    lines = spark.sparkContext.textFile("u.data")

    movies = lines.map(parseInput)
    df = spark.createDataFrame(movies)
    print(df.show(5, False))
