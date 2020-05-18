public class abstract_child_class_super_keyword extends abstract_parent_class
{
	
int a = 10;

abstract_child_class_super_keyword()
{
	System.out.println(" abstract_child_class inside-non-parameterized constructor");
}

 public void subtraction()
{
 System.out.println("implementing abstract subtract method");	
 System.out.println(super.a);   // accessing parent class variable 

   super.addition();

 
}

public static void main(String args[])
{
	abstract_child_class_super_keyword c1 = new abstract_child_class_super_keyword();
	c1.addition();  // using non-abstract method of abstract class
	System.out.println(" printing variable same as parent class " + c1.a);
	c1.subtraction();  // using abstract method

	
	abstract_child_class_super_keyword c2 = new abstract_child_class_super_keyword();
	c2.addition();
   //when we create object of the child class then automatically call
   // parent class constructor using super
	
}

}
 


//super keyword = when parent class and child class has same memebers
// we can use super keyword toaccess parent class members
//to explicitly call non-parmeterized and parameterized constructor
// when we override method in child class , if we want to access parent method
// we can use  super keyword
// we cannot use super in static methods



