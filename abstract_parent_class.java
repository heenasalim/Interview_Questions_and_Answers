
// What is abstraction = Only implementing necessory methods are know as abstraction
// example :- if user want to withdraw money he need to provide implemetation to
// addpin(), addamount(), swipeATM() methods else 
// he cannot withdraw money
// he dont need to know what addamount() , swipeATM(), addpin() works

//abstract class  -- class which has keyword abstract
// it has abstract as well as non-anstract methods
// abstract methods means empty methods
// non -abstract memthods are methods which are non-empty
// we cannot create object of abstract class
// we need to extend abstract class to create object .
// and provide implementation to all abstract methods of abstract class
// then only we can use non-abstract methods


//abstract method - abstract method has abstract keyword;
// we can define abstract methods only in abstract class
// we can implement abstract method only child class of abstract class
//abstract class can constructor



//Traits are similar to abstract class in scala
// it has implemeted as well no implemeted methods
// it has abstract as well as non absract methods
// but in order to make use of non-abstract method user need to extend trait class





 public abstract class abstract_parent_class
{
int a = 20;

abstract_parent_class()
{
	
	System.out.println(" abstract_parent_class non-parameterized constructor ");
}
	
public void addition()   //non-abstract method
{ System.out.println("we are inside addition method");}
	
abstract void subtraction() ; //abstract method


/*public static void main(String args[])
{

	
	//abstract_class l = new abstract_class();
	
	// we cannot create object of abstract class
}
*/
	
}

