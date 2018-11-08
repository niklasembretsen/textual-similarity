import scala.io.Source
import java.security.MessageDigest
import scala.collection.mutable.TreeSet
import java.nio.ByteBuffer

object Shingling {

	def main(args:Array[String]){
		val docSet1 = shingling("test.txt", 2)
		val docSet2 = shingling("test2.txt", 2)
		val universalSet: TreeSet[Int] = docSet1.union(docSet2)
		val comparison = jaccardSimilarity(docSet1, docSet2)
		println(comparison)
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

	def minHash (hashedShingles: TreeSet[Int], n: Int): Vector[Int] = {
		// generate n hash functions on the form (ax + b) % c
		// where a is a random variable, , x the hashed value (shingle)
		// and c the number of shingles
		
		val c: Int = hashedShingles.size
		val r = new scala.util.Random(c)
		val aValues: Vector[Int] = Vector.empty
		//val result: Seq[Int] = for(i <- 0 to c) yield r.nextInt(c)
		val result = for(i <- 1 to c) yield {
    			r.nextInt(c)
		}
		println(result)
		aValues
		//val signaturevector : Vector[Int] = Vector.zeros(c)
	}

}