class LandingReader:

    def __init__(self, spark, landing_path):
        self.spark = spark
        self.landing_path = landing_path

    def read(self, source, dataset):
        path = f"{self.landing_path}/{source}/{dataset}"

        return (
            self.spark.read
            .option("multiline", "true")
            .json(path)
        )