import scala.collection.mutable.ListBuffer

object CountTree{
  val cntLevel:ListBuffer[List[List[Int]]] = ListBuffer()
  def getCurrentLevel = cntLevel.size
  def addNewLevel(levelList:List[List[Int]]){
    cntLevel += levelList
  }
  def calcNextItemSet:List[List[Int]] = {
    val curLayer = cntLevel(getCurrentLevel-1)
    val initLayer = cntLevel(0)
    val nxtSet = for{
      x <- 0 until curLayer.size
      y <- 0 until initLayer.size
      if(curLayer(x).last<initLayer(y)(0))
    }
    yield(curLayer(x) :+ initLayer(y)(0))
    nxtSet.toList
  }
  
  def writeToFile(fname:String){
    val pw = new java.io.PrintWriter(fname)
    var cnt = 1
    cntLevel.foreach{
      x =>
        val words = x.map(_.mkString("-"))
        val line = words.mkString(" ")
        pw.println("( "+cnt+" ) "+line)
        cnt += 1
    }
    pw.close
  }
  
}