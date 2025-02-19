// //> using scala 3.3.5
// //> using dep org.typelevel::cats-effect:3.5.7
// //> using dep org.mongodb:mongodb-driver-sync:5.3.1
// import java.io.File
// import scala.io.Source

// trait IntegratorSource[T]

// trait CSVSource[T] extends IntegratorSource[T] {
//     def location: String
//     def read: List[List[String]] = {
//         val file = new File(location)
//         Source.fromFile(file).getLines().map(_.split(",").map(_.trim).toList).toList
//     }
// }

// trait MongoSource[T] extends IntegratorSource[T] {
//     def collection: String
//     def connectionString: String

//     def read: List[T]
// }

// trait IntegratorSink[T]

// trait MongoSink[T] extends IntegratorSink[T] {
//     def collection: String
//     def connectionString: String

//     def write(data: List[T]): Unit
// }

// trait CSVSink[T] extends IntegratorSink[T] {
//     def location: String

//     def write(data: List[T]): Unit
// }

// trait Integrator[S, D] {
//     def source: IntegratorSource[S]
//     def sink: IntegratorSink[D]

//     def run: Unit
// }