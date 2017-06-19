package de.tu_berlin.dima.bdapro.datagen.flink

import de.tu_berlin.dima.bdapro.datagen.util.RanHash

class Dictionary (seed : Long, size : Int) extends Iterable[String] {

  private val MIN_LENGTH = 2
  private val MAX_LENGTH = 16
  private val NUM_CHARACTERS = 26

  private val random : RanHash = new RanHash(seed)

  override def iterator: Iterator[String] = words.iterator

  def word (index : Int) : String = {
    require(0 <= index && index < size)
    // skip to the correct position within the random sequence
    random.skipTo(index * (NUM_CHARACTERS + 1))
    val len = random.nextInt(MAX_LENGTH - MIN_LENGTH - 1) + MIN_LENGTH
    val strBld = new StringBuilder(len)
    for (i <- 0 until len) {
        val c = ('a'.toInt + random.nextInt(NUM_CHARACTERS)).toChar
        strBld.append(c)
      }
    strBld.mkString
  }

  def words () : Array[String] = (0 until size).map(i => word(i)).toArray
}
