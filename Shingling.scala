import scala.io.Source
import java.security.MessageDigest
import scala.collection.mutable.TreeSet
import java.nio.ByteBuffer
import scala.util.Random

object Shingling {

	def main(args:Array[String]){
		val docSet1 = shingling("test.txt", 2)
		val docSet2 = shingling("test2.txt", 2)
		val universalSet: TreeSet[Int] = docSet1.union(docSet2)
		val universalMap: Map[Int, Int] = universalSet.zipWithIndex.toMap
		val comparison = jaccardSimilarity(docSet1, docSet2)
		val test = minHash(universalMap, docSet1, 10)
		val test2 = minHash(universalMap, docSet2, 10)
		println(comparison)
		println(docSet1)
		print(docSet2)
		println(test)
		println(test2)
	}

	def jaccardSimilarity (document1: TreeSet[Int],document2: TreeSet[Int]): Double = {
		//compute the Jaccard similarity (A ∩ B) / (A U B)
		val intersection: Double = document1.intersect(document2).size
		val union: Double = document1.union(document2).size
		intersection / union
	}

	def shingling (filename: String, k: Int): TreeSet[Int] = {

		//A set for holding the shingles of a document
		var documentShingles: Set[String] = Set()

		//fetch each line from the specified file
		for(line <- Source.fromFile(filename).getLines) {
			//get the length of the line to know how far to go when
			//creating the k-shingles of the line. Finally add
			//each shingle to the set.
			var lineLength = line.length
			for(i <- 0 to (lineLength - k)) {
				var shingle = line.slice(i, i + k)
				documentShingles += shingle
			}
		}
		val hashedSingles = documentShingles
			.map(word => word.hashCode())

		val orderedHashedShingles = TreeSet[Int]() ++ hashedSingles
		orderedHashedShingles
	}

	def minHash (hashedShingles: Map[Int, Int], document: TreeSet[Int], n: Int): Seq[Int] = {
		// generate n hash functions on the form (ax + b) % c
		// where a and b are random variables, x the hashed value (shingle)
		// and c the number of shingles
		Random.setSeed(10)
		//var signatureVector: Set[Int] = Set()
		var signatureVector = for(i <- 1 to n) yield {
    			Int.MaxValue
		}

		val c: Int = hashedShingles.size
		val r = new Random(c)

		val aValues = for(i <- 1 to n) yield {
    			r.nextInt(c)
		}

		val bValues = for(i <- 1 to n) yield {
    			r.nextInt(c)
		}

		//var rowNumber = 0
		for (value <- document) {
			val rowNum = hashedShingles.get(value).getOrElse(-1)
			//the documents contains hash from row
			if (rowNum >= 0) {

				for (i <- 0 to (n - 1)) {
					val sigHash = (aValues(i) * rowNum + bValues(i)) % c
					if (signatureVector(i) > sigHash) {
						signatureVector updated (i, sigHash)
					}
				}
			}
		}
		signatureVector
	}

}