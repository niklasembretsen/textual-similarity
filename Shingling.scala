import scala.io.Source

object Shingling {

	def main(args: Array[String]) {
		shingling("test.txt", 2)
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

		documentShingles.foreach(println)
	}
}