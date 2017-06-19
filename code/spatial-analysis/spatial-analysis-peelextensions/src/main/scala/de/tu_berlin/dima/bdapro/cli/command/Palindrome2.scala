package de.tu_berlin.dima.bdapro.cli.command

import scala.io.Source

/**
  * Created by JML on 10/25/16.
  */
object Palindrome2 {
  def main(args: Array[String]): Unit = {
    // Do not consider newline character

    // CONSIDER the same length of sentence
    val filePath = "/jml/data/d/1 CLASS STUDY/1 Lecture/1 IT4BI Second/3 BDA Project/3 Lab/mybdapro/test/input.txt"
    try{
      val lineList = Source.fromFile(filePath).getLines().toList
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
          val result3 = lineList.reduceLeft((x,y) => compareSentence(x,y))
          if(result3.compareToIgnoreCase("") == 0){
            println("There is no palindrome sentence in file")
          }
          else {
            println("Longest palindrome 3: " + result3)
          }
        }
      }

    }
    catch {
      case ex:Exception => println(ex)
    }


    /**
      * TEST HERE
      */

    println(isPalindromeSentence("hello world akaka"))

    println(isPalindromeSentence("abc abc cba cba"))

    // sample code checking array of sentences. Be careful with case only one sentence in file
    var sentenceVector1 = Vector("abc cba 33")
    var result = sentenceVector1.reduceLeft((x,y) => compareSentence(x,y))
    println("Longest palindrome 1: " + result)

    var sentenceVector2 = Vector("abc cba 33", "abc abc cba cba", "4 qwer fg gf rewq 4", "abcde abcde edcba edcba 33")
    var result2 = sentenceVector2.reduceLeft((x,y) => compareSentence(x,y))
    println("Longest palindrome 2: " + result2)


  }


  def isPalindromeSentence(sentence:String):Boolean = {
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
}
