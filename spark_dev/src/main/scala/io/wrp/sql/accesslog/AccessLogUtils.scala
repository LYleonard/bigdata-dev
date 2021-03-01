package sql.accesslog

import scala.util.matching.Regex

/**
  * @Author LYleonard
  * @Date 2020/5/16 15:44
  * @Description 通过sparksql对用户访问产生点击流日志数据进行分析处理，计算出对应的指标
  */

case class AccessLog(
                    ipAddress: String, //IP地址
                    clientId: String,  //客户端唯一标识符
                    userId: String,    //用户唯一标识符
                    serverTime: String, //服务器时间
                    method: String,     //请求类型/方式
                    endPoint: String,   //请求的资源
                    protocol: String,   //请求的协议名称
                    responseCode: Int,  //请求返回值：比如：200、401
                    contentSize: Long,   //返回的结果数据大小
                    url: String,           //访问的url地址
                    clientBrower: String   //客户端游览器信息
                    )

/**
  * 校验日志数据进行字段解析提取的工具类
  */
object AccessLogUtils {
  val regex: Regex = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+) (\S+) (.*)""".r

  /**
    * 验证一下输入的数据是否符合给定的日志正则，如果符合返回true；否则返回false
    * @param line
    * @return
    */
  def isValidateLogLine(line: String): Boolean = {
    val options = regex.findFirstMatchIn(line)

    if (options.isEmpty){
      false
    } else {
      true
    }
  }

  def parseLogLine(line: String): AccessLog = {
    //从line中获取匹配的行
    val options = regex.findFirstMatchIn(line)
    //获取matcher
    val matcher = options.get

    //返回值
    AccessLog(
      matcher.group(1), //获取匹配字符串中第一个小括号中的值
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8).toInt,
      matcher.group(9).toLong,
      matcher.group(10),
      matcher.group(11)
    )
  }
}
