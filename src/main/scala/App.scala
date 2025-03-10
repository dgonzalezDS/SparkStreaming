

import org.apache.spark.sql.SparkSession

/*

Para ejecutar un ejecicio concreto, es necesario pasar como parametros de ejecucion el número del capitulo y el número de ejercicio
Por ejemplo, si queremos ejecutar el ejercicio 2 del capitulo 3, deberemos pasar  3 2  como parametros de ejecucion

Tambien es posible ejecutar los ejercicios desde la terminal. Para ello, es necesario usar los comandos mvn exec, como se especifica al inicio del ejercicio
 */

object App {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Uso: mvn exec:java -Dexec.mainClass=\"App\" -Dexec.args=\"<capitulo> <ejercicio>\"")
      println("Ejemplo: mvn exec:java -Dexec.mainClass=\"App\" -Dexec.args=\"2 1\"")
      sys.exit(1)
    }

    val capitulo = args(0)
    val ejercicio = args(1)

    // Crear SparkSession una única vez para utilizarlo en cada capitulo/ejercicio
    val spark: SparkSession = SparkSession
      .builder
      .appName("EjerciciosScala")
      .master("local[*]")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    println(s"Ejecutando capítulo $capitulo, ejercicio $ejercicio")

    val chapterNumber = args(0)
    val exerciseNumber = args(1)

    val clazz = Class.forName(s"chapter$chapterNumber$$")
    val module = clazz.getField("MODULE$").get(null)
    val methodName = s"ejercicio$exerciseNumber"
    val exerciseMethod = clazz.getMethod(methodName) // El ej no necesita ", classOf[SparkSession]" como argumento
    exerciseMethod.invoke(module) // Los ejercicios 1,2,3 necesitan que le pasemos la spark session, el 4 no

    // Cerrar SparkSession al finalizar
    spark.stop()
  }
}

