import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object SocketWordCount {
  def main(args: Array[String]): Unit = {

    // Создаем Flink Stream Execution Environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Читаем данные из сокета (локальный хост, порт 9999)
    val text = env.socketTextStream("localhost", 9999)

    // Преобразуем поток: разбиваем текст на слова и считаем
    val wordCounts = text
      .flatMap(new LineSplitter())
      .keyBy(0) // Группировка по слову (первый элемент Tuple2)
      .sum(1)   // Суммируем количество (второй элемент Tuple2)

    // Выводим результат на консоль
    wordCounts.print()

    // Запускаем Flink приложение
    env.execute("Socket WordCount")
  }

  // Класс для разбивки строк на слова
  class LineSplitter extends FlatMapFunction[String, Tuple2[String, Int]] {
    override def flatMap(value: String, out: Collector[Tuple2[String, Int]]): Unit = {
      // Разбиваем строку на слова и собираем результат
      value.split("\\s+").foreach(word => out.collect(new Tuple2(word, 1)))
    }
  }
}
