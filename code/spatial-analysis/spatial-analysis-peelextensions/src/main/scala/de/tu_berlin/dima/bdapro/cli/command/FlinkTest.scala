package de.tu_berlin.dima.bdapro.cli.command


import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by JML on 10/26/16.
  */
object FlinkTest {

  def main(args: Array[String]): Unit = {
    val filePath = "/jml/data/d/1 CLASS STUDY/1 Lecture/1 IT4BI Second/3 BDA Project/3 Lab/mybdapro/test/input.txt"
    val filePath2 = "/jml/data/input.txt"
    val outputFilePath = "/Users/JML/output"
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val text: DataStream[String] = env.readTextFile(filePath2)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(filePath2)

//    val result = text.flatMap(x => x.split("\n")).keyBy(x => isPalindromeSentence(x)).reduce((x,y)=>compareSentence(x,y))

    val palindromeList = text.flatMap{_.split("\n")}
      .filter(x=> isPalindromeSentence(x))
    val palindromeMap = palindromeList.map(x=>(x.length, x))
    val maxPalindrome = palindromeMap.maxBy(0)

    val groupByList = palindromeMap.groupBy(0)
    val sortedGroup = groupByList.sortGroup(0, Order.DESCENDING)
    val maxLength = 0;
    var listBufferNew = new ListBuffer[String]

    palindromeList.collect().map(x => recursiveCompare2(x, listBufferNew))

//    palindromeList.flatMap(x => recursiveCompare2(x,listBufferNew))

    for(x <- listBufferNew){
      println("Hellow: " + x)
    }

    val result = sortedGroup.first(1)
    result.print()
    result.writeAsCsv(outputFilePath, "\n", ",", FileSystem.WriteMode.OVERWRITE)
//    val palindromeList = result.filter(x=>isPalindromeSentence(x))
//    palindromeList.keyBy(x => x.length).fold(new ListBuffer[String], new FoldFunction<ListBuffer[String], String> {
//
//    })
//    val maxSenList = new ListBuffer[String]()
//    palindromeList.map(x => recursiveCompare2(x, maxSenList))
//    val maxSenArr = maxSenList.toList
//    maxSenArr.foreach(x => "Hehehe" + x)

    println("Here......")
    println("End Here......")
    env.execute()

    try{
      val lineList = Source.fromFile(filePath2).getLines().toList
      if(lineList.isEmpty){
        println("There is no sentence in file")
      }
      else{
        if(lineList.size == 1){
          if(isPalindromeSentence(lineList.head)){
            println("Longest palindrome: " + lineList.head)
          }
          else{
            println("There is no palindrome sentence in file")
          }
        }
        else{
//          val result3 = lineList.reduceLeft((x,y) => compareSentence(x,y))
//          if(result3.compareToIgnoreCase("") == 0){
//            println("There is no palindrome sentence in file")
//          }
//          else {
//            println("Longest palindrome 3: " + result3)
//          }
//            val buffer = ListBuffer[String]
//            val result3 = lineList.fol
          val groupBy = lineList.groupBy(x=>x.length)
          val group5 = groupBy.get(16)
            group5.foreach(x => println("Hic hic" + x))
        }
      }

      val listBuffer2 = new ListBuffer[String]
      val abc = List("10", "1", "2", "3", "11")
      recursiveCompare(abc, 0, listBuffer2)
      listBuffer2.toList.foreach(x => println("Testmax: " + x))

    }
    catch {
      case ex:Exception => println(ex)
    }
  }

  def isPalindromeSentence(sentence:String):Boolean = {
    // TODO: use reverse in recursive way
    // 1.Remove space
    var newsentence = sentence.replaceAll(" ", "")
    // val sentenceList = newsentence.toList
    // 2.Reverse sentence
    // var reversedList = sentenceList.reverse
    // 3.Create reversed sentence
    //var reversedSen = reversedList.map(_.toString).reduceLeft((a,b)=>a.toString + b.toString)
    val reversedSen = newsentence.reverse

    // 4.Compare sentence and reversed one
    if(newsentence.compareToIgnoreCase(reversedSen) == 0)
      true
    else
      false
  }

  def compareSentence(x:String, y:String):String = {

    if(isPalindromeSentence(x) && isPalindromeSentence(y)){
      if(x.trim.length > y.trim.length)
        x
      else
        y
    }
    else{
      if(isPalindromeSentence(x))
        x
      else
      {
        if(isPalindromeSentence(y))
          y
        else
          ""
      }
    }
  }

  def recursiveCompare(sentenceList:List[String], currentMaxLength:Int, maxLengthSenList:ListBuffer[String]): Unit ={
    if(!sentenceList.isEmpty){
      var headSentence = sentenceList.head;
      if(headSentence.length > currentMaxLength){
        maxLengthSenList.clear();
        maxLengthSenList += headSentence;
        return recursiveCompare(sentenceList.tail, headSentence.length, maxLengthSenList);
      }
      else{
        if(headSentence.length == currentMaxLength){
          maxLengthSenList += headSentence;
          return recursiveCompare(sentenceList.tail, currentMaxLength, maxLengthSenList);
        }
        else{
          return recursiveCompare(sentenceList.tail, currentMaxLength, maxLengthSenList);
        }
      }
    }
  }

  def recursiveCompare2(sentence:String, maxLengthSenList:ListBuffer[String]): String ={
    if(maxLengthSenList.isEmpty){
      maxLengthSenList += sentence
    }
    else{
      var headSentence = maxLengthSenList.head
      if(headSentence.length == sentence.length){
        maxLengthSenList += sentence
      }
      else{
        if(headSentence.length < sentence.length){
          maxLengthSenList.clear()
          maxLengthSenList += sentence
        }

      }
    }
    return sentence
  }

  def maxBy[A, B](l: Seq[A])(f: A => B)(implicit cmp: Ordering[B]) : Seq[A] = {
    l.foldLeft(Seq.empty[A])((b, a) =>
      b.headOption match {
        case None => Seq(a)
        case Some(v) => cmp.compare(f(a), f(v)) match {
          case -1 => b
          case 0 => b.+:(a)
          case 1 => Seq(a)
        }
      }
    )
  }





}
