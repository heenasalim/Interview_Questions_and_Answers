
            //How to create of the Scala Class)
//we created the class added the one method and main method in class

class Class_Creation {  //* Class 
  def add()=
 {
   var a =10;
   var b =11;
   
   println(s"${a+b}");
 }
}

//class and object should be totally separatly defined
//Return type of the main is Unit only 

object createclass{
 def main(args:Array[String]):Unit =
 {
  lazy val c = new Class_Creation() 
  //memory will be allocated at computation time only
  //variables,new object is define by the val/var
  println(c.add);
 }
 }

