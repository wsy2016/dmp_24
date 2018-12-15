val list1=List(1,2,3)
val list2=List(3,4,5)
val list3=List(5,6,7)

val step1=list1.zip(list2)
step1.map(t=>t._1+t._2)
