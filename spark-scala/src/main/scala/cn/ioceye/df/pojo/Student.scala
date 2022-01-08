package cn.ioceye.df.pojo

/**
 * 注意：RDD与DataFrame之间的转换,必须为case class.
 * case class
 * 1.默认是可以序列化的, 也就是实现了Serializable
 * 2.默认实现了equals和hashCode函数
 * 3.支持模式匹配
 * 4.初始化的时候可以不用new,也可以加上,普通类一定需要加new
 */
case class Student(id: Int, name: String, age: Int)