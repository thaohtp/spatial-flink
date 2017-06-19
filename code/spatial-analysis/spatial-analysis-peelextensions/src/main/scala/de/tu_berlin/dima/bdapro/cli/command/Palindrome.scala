package de.tu_berlin.dima.bdapro.cli.command


import java.lang.Iterable

import org.apache.flink.api.common.functions.{GroupCombineFunction, RichGroupCombineFunction}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by JML on 11/6/16.
  */
object Palindrome {
  def main(args: Array[String]): Unit = {
    val inputFilePath = "file:///jml/data/input.txt"

    // set up environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.readTextFile(inputFilePath)

    // filter palindrome sentences
    val palindromeSet = dataSet.flatMap(_.split("\n")).filter(x => isPalindromeSentence(x))
      .map(x => (x.replaceAll(" ", "").length, x))



    val biggestPalindromeSet = palindromeSet
        .combineGroup(new GroupCombineFunction[(Int,String),(String)] {
          override def combine(iterable: Iterable[(Int, String)], collector: Collector[(String)]): Unit = {
                    var lengthMax = 0
                    var strList = new ListBuffer[String]
                    for ((lengthCurr, strCurr) <- iterable.asScala) {
                      if (lengthCurr >= lengthMax) {
                        lengthMax = lengthCurr
                        strList += strCurr
                      }
                    }
            // get biggest sentence
                    for (str <- strList.toList) {
                      if (str.replaceAll(" ", "").length == lengthMax) {
                        collector.collect(str)
                      }
                    }
          }
        })

    // get biggest sentences
//    val biggestPalindromeSet = palindromeSet.reduceGroup {
//      (in, out: Collector[(String)]) => {
//        // calculate max length
////        var lengthMax = 0
////        var strList = new ListBuffer[String]
////        for ((lengthCurr, strCurr) <- in) {
////          if (lengthCurr > lengthMax) {
////            lengthMax = lengthCurr
////            strList += strCurr
////          }
////        }
////
////        // get biggest sentence
////        for (str <- strList.toList) {
////          if (str.replaceAll(" ", "").length == lengthMax) {
////            out.collect(str)
////          }
////        }
//        var lengthMax = 0
//        var strList = new ListBuffer[String]
//        for((lengthCurr, strCurr) <- in ){
//            if(lengthCurr > lengthMax){
//              lengthMax = lengthCurr
//              strList.clear()
//              strList += strCurr
//            }
//            else {
//              if(lengthCurr == lengthMax){
//                strList += strCurr
//              }
//            }
//        }
//        for(str <- strList){
//          out.collect(str)
//        }
//
//      }
//    }
    // Print result
    biggestPalindromeSet.map(str => "The biggest palindrome sentence: <" + str + ">").print()

    //          .reduceGroup{
    //            (in, out: Collector[(String)]) =>
    //            {
    //              var lengthPrev = 0
    //              for((lengthCurr, strCurr ) <- in){
    //                if(lengthPrev == 0){
    //                  lengthPrev = lengthCurr
    //                  out.collect(strCurr)
    //                }
    //                else{
    //                  if(lengthPrev == strCurr.length){
    //                    out.collect(strCurr)
    //                  }
    //                }
    //              }
    //
    //            }
    //          }

    //    val palindromeSeq = palindromeSet.collect()
    //    palindromeSeq.map(x => recursiveCompare(x, resultBuffer))
    //    resultBuffer.foreach(x => println("Result: " + x))
    //    palindromeSet.filter(x => resultBuffer.toList.contains(x))

    //    palindromeSet.map { x => "Palindrome sentence: " + x }.writeAsText(outputFilePath, FileSystem.WriteMode.OVERWRITE)
    //    env.execute("Palindrome")


    // Solution from Gabor
    /**
      * Find maxlength first with .max(1).collect().head._2
      * Then put maxlength to compare in filter function
      *
      * Another solution:
      * If we use cross, it can be the same withBroadcastSet because in this case, they only have one element(maxLength) to cross
      *
      * Solution 3: we can use reduce function to calculate max length
      * Solution 4: if we use reduceGroup with the iterator of whole group, we should use combine group
      */

  }

  def isPalindromeSentence(sentence: String): Boolean = {
    // TODO: use reverse in recursive way
    // 1.Remove space
    val newSentence = sentence.replaceAll(" ", "")
    // val sentenceList = newsentence.toList
    // 2.Reverse sentence
    // var reversedList = sentenceList.reverse
    // 3.Create reversed sentence
    val reversedSentence = newSentence.map(_.toString).reduceRight((a, b) => b.toString + a.toString)
    //    val reversedSen = newsentence.reverse
    // 4.Compare sentence and reversed one
    if (newSentence.compareToIgnoreCase(reversedSentence) == 0)
      true
    else
      false
  }


  def recursiveCompare(sentence: String, maxLengthSenList: ListBuffer[String]): String = {
    if (maxLengthSenList.isEmpty) {
      maxLengthSenList += sentence
    }
    else {
      val headSentence = maxLengthSenList.head
      if (headSentence.length == sentence.length) {
        maxLengthSenList += sentence
      }
      else {
        if (headSentence.length < sentence.length) {
          maxLengthSenList.clear()
          maxLengthSenList += sentence
        }

      }
    }
    return sentence
  }
}
