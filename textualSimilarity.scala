import scala.io.Source
import java.security.MessageDigest
import scala.collection.mutable.TreeSet
import java.nio.ByteBuffer
import java.io.File
import scala.util.Random
import java.lang.Math
import scala.util.hashing.MurmurHash3

object TextualSimilarity {

	def main(args:Array[String]){

		val directory = "documents"

		var documents: Seq[TreeSet[Int]] = Seq()
		var universalSet: TreeSet[Int] =TreeSet()

		//size of the shingles
		val k = 5
		//length of signature (should be cubic)
		val n = 64

		val files = new File(directory).listFiles

		for(file <- files){
			val path: String = directory + "/" + file.getName
			val docSet = shingling(path, k)
			documents = documents :+ docSet
			universalSet = universalSet ++ docSet
		}

		val universalMap: Map[Int, Int] = universalSet.zipWithIndex.toMap

		// val comparison = jaccardSimilarity(docSet1, docSet2)
		// val sign1 = minHash(universalMap, docSet1, n)
		// val sign2 = minHash(universalMap, docSet2, n)
		// val signatureSeq = Seq(sign1, sign2)
		// val frac = compareSignatures(sign1, sign2)
		// val similarDocs = doLSH(0.5,n, signatureSeq)
		// println(similarDocs)

		//OBS hardcoded r and b in getRandB
		println(testFunc3(documents, universalMap, n, 0.6))
		//println(testFunc2(documents, universalMap, n))

	}

	//Only Jaccard similarity between all documents
	def testFunc1(documentShingles: Seq[TreeSet[Int]]): List[(Int, Int, Double)] = {
		//create a list with document IDs (0 -> numDocs - 1) for
		//indexing the documents
		val docIDs: List[Int] =  List.range(0, documentShingles.size)
		val documentPairs: List[List[Int]] = docIDs.combinations(2).toList

		val docSimilarity: List[(Int, Int, Double)] =
			documentPairs
			.map(pair => {
					val doc1 = documentShingles(pair(0))
					val doc2 = documentShingles(pair(1))
					val comparison: Double = jaccardSimilarity(doc1, doc2)
					(pair(0), pair(1), comparison)
					}
				).toList
		docSimilarity
	}

	//Using minHasing
	def testFunc2(documentShingles: Seq[TreeSet[Int]], universalMap: Map[Int, Int], n: Int): List[(Int, Int, Double)] ={

		val documentSignatures: Seq[Seq[Int]] =
			documentShingles.map(document => {
				minHash(universalMap, document, n)
			})

		val docIDs: List[Int] =  List.range(0, documentShingles.size)
		val documentPairs: List[List[Int]] = docIDs.combinations(2).toList

		val docSimilarity: List[(Int, Int, Double)] =
			documentPairs
			.map(pair => {
					val doc1 = documentSignatures(pair(0))
					val doc2 = documentSignatures(pair(1))
					val comparison: Double = compareSignatures(doc1, doc2)
					(pair(0), pair(1), comparison)
					}
				).toList
		docSimilarity
	}

	//Using LSH
	def testFunc3(documentShingles: Seq[TreeSet[Int]], universalMap: Map[Int, Int], n: Int, threshold: Double): List[(Int, Int, Double)] = {

		val documentSignatures: Seq[Seq[Int]] =
			documentShingles.map(document => {
				minHash(universalMap, document, n)
			})

		doLSH(threshold, n, documentSignatures).toList
	}

	/** Computes the Jaccard similarity between two documents of hashed shingles
   *
   *  @param document1: the first document
   *  @param document2: the second document
   *  @return the Jaccard similarity between the documents
   */
	def jaccardSimilarity (document1: TreeSet[Int],document2: TreeSet[Int]): Double = {
		//compute the Jaccard similarity (A ∩ B) / (A U B)
		val intersection: Double = document1.intersect(document2).size
		val union: Double = document1.union(document2).size
		intersection / union
	}

	/** Creates the (unique) k-shingles for a document
   *
   *  @param filename: the file from where the shingles are created
   *  @param k: the size of the shingles
   *  @return the set of unique k-shingles contained in the document
   */
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
		//hash each shingle from the document
		val hashedSingles = documentShingles
			.map(word => word.hashCode())

		//represent the hashed shingles as an ordered set
		val orderedHashedShingles = TreeSet[Int]() ++ hashedSingles
		orderedHashedShingles
	}

	/** Computes the minHash signature for a document
	*
	*  @param hashedShingles: the universal set of shingles
	*			(contained in the body of documents)
	*  @param document: the document to compute the signature for
	*  @param n: the length of the signature
	*  @return the minHash signature of the document
	*/
	def minHash (hashedShingles: Map[Int, Int], document: TreeSet[Int], n: Int): Seq[Int] = {
		// generate n hash functions on the form (ax + b) % c
		// where a and b are random variables, x the hashed value (shingle)
		// and c the number of shingles
		Random.setSeed(10)
		//Init the signature vector with large value
		var signatureVector = for(i <- 1 to n) yield {
    			Int.MaxValue
		}
		//number of rows of the "characteristic matrix"
		val c: Int = hashedShingles.size
		val r = new Random(c)
		//generate params for the hash functions
		val aValues = for(i <- 1 to n) yield {
    			r.nextInt(c)
		}

		val bValues = for(i <- 1 to n) yield {
    			r.nextInt(c)
		}

		for (value <- document) {
			//get the row number of the shingle from the universal set ("Characteristic matrix")
			val rowNum = hashedShingles.get(value).get

			//for each row, generate n hash values and update signature with
			// the smallest value
			for (i <- 0 to (n - 1)) {
				val sigHash = (aValues(i) * rowNum + bValues(i)) % c
				if (signatureVector(i) > sigHash) {
					signatureVector = signatureVector updated (i, sigHash)
				}
			}
		}
		signatureVector
	}

	/** Compare two signatures by computing commonComponents/numComponents
	*
	*  @param signature1: A signature
	*  @param signature2: A signature
	*  @return The fraction of components the signatures have in common
	*/
	def compareSignatures(signature1:Seq[Int], signature2:Seq[Int]): Double = {
		val fraction: Double = (signature1.intersect(signature2)).size
		val numberComponents: Double = signature1.size
		fraction / numberComponents
	}

	/** Perform Locality Sensitive Hashing (LSH)
	*
	*  @param threshold: How common the documents (signatures) have to be
	*  @param n: length of a signature
	*  @param signatures: the documents (signatures) to compare
	*  @return the similar document pairs as well as their similarity, as:
	*			(docID, docID, similarity)
	*/
	def doLSH(threshold: Double, n: Int, signatures: Seq[Seq[Int]]): Seq[(Int, Int, Double)] = {
		//Get the values of r and b given a signature length for 
		//LSH-threshold ~0.5
		val rAndB: (Int, Int) = getRandB(n)
		val r: Int = rAndB._1.toInt
		val b: Int = rAndB._2.toInt

		//The set for holding candidate pairs
		var canidatePairs: Set[(Int, Int)] = Set()

		//Loop through bands and compute the hash for each signature
		for (band <- 0 to (b - 1)){
			//Represent the buckets as a map of (hashedValue => List(docIDs))
			var buckets: Map[Int, List[Int]] = Map()
			var docId = 0
			//for each band, compute the hash for all signatures
			//and add to buckets
			for (signature <- signatures) {
				val startIndex = band * r
				val endIndex = startIndex + r
				val slicedSignature = signature.slice(startIndex, endIndex)
				val bucketNumber = MurmurHash3.seqHash(slicedSignature)

				val documentList: List[Int] = buckets.get(bucketNumber).getOrElse(List())
				documentList match {
					case List() => {
						//Bucket is empty, add document
						buckets = buckets + (bucketNumber -> List(docId))
					}
					case _ => {
						//Bucket is not empty, add document to list
						val newList: List[Int] = documentList :+ docId
						buckets = buckets + (bucketNumber -> newList)
					}
				}
				docId += 1
			}
			//Get all buckets containing a document
			val bucketContents = buckets.values.toList

			for (bucketContent <- bucketContents) {
				//check if bucket has more than one document
				bucketContent.size match {
					case 1 =>
					case _ => {
						//if bucket has more than one document, add to 
						//candidate pairs
						val pairs = getCandidatePairs(bucketContent)
						canidatePairs = canidatePairs ++ pairs
					}
				}
			}
		}
		//Compare signatures for all candidate pairs and
		//keep if similarity > threshold
		val similarDocuments: Seq[(Int, Int, Double)] =
			canidatePairs
			.map(x => {
				val doc1ID = x._1
				val doc1 = signatures(doc1ID)
				val doc2ID = x._2
				val doc2 = signatures(doc2ID)
				val similarity : Double = compareSignatures(doc1, doc2)
				(doc1ID, doc2ID, similarity)
			}).toSeq.filter( tuple => tuple._3 > threshold)
		similarDocuments
	}

	/** Get the r (rows) and b (bands) for LSH given a signature lenhgth
	* using the relationship b = r^2 as this yeilds LSH-threshold of
	* approximately 0.5 in the range of n we use (8 (2^3) <= n <= 216 (6^3))
	*
	*  @param n: the length of the signatures
	*  @return r and b
	*/
	def getRandB(n: Int): (Int, Int) = {
		//n (length of signature) has to be cubic => lsh-threshold ~ 0.5
		val r: Int = Math.cbrt(n).toInt
		val b : Int = Math.pow(r,2).toInt
		(2, 32)
	}

	/** Generate unique pairs from a list of elements
	*
	*  @param documentList: list of documentIDs
	*  @return a set of unique pairs from the list
	*/
	def getCandidatePairs(documentList: List[Int]): Set[(Int, Int)] = {
		val pairs: Set[(Int, Int)] = documentList.combinations(2)
					.toList
					.map(x => (x(0), x(1)))
					.toSet
		pairs
	}
}