import scala.io.Source
import java.security.MessageDigest
import scala.collection.mutable.TreeSet
import java.nio.ByteBuffer

object Shingling {

	def main(args: Array[String]) {
		shingling("test.txt", 5)
	}

	def shingling(filename: String, k: Int) {

		//A set for holding the shingles of a document
		var documentShingles: Set[String] = Set()

		//fetch each line from the specified file
		for (line <- Source.fromFile(filename).getLines) {
			//get the length of the line to know how far to go when
			//creating the k-shingles of the line. Finally add
			//each shingle to the set.
			var lineLength = line.length
			for (i <- 0 to (lineLength - k)) {
				var shingle = line.slice(i, i + k)
				documentShingles += shingle
			}
		}
		val hashedSingles = documentShingles
			.map(word => word.hashCode())

		val orderedHashedShingles = TreeSet[Int]() ++ hashedSingles
		documentShingles.foreach(println)
		orderedHashedShingles.foreach(println)
	}
}