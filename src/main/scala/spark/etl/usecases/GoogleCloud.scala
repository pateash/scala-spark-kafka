package spark.etl.usecases

import spark.etl.InitSpark

object GoogleCloud extends InitSpark {

  def main(args: Array[String]) = {

    // setting GCS Auth Config
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/a0p0a59/Desktop/docker/airflow/plugins/keys/wmt-core-ip-us-dev-afeb1b8048af.json")

    val df=spark.read
      .option("inferSchema",value = true)
      .parquet("gs://wmt_core_ip_us_stg/warehouse/ip_us_stg.db/rdc_ob_trailer_actuals_summary/calendar_date=2021-05-14")

    df.show(false)
    print(df.count())
    close
  }


}
