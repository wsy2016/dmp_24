import java.util.UUID

import cn.dmp.utils.ConfUtils

object Demo {
  def main(args: Array[String]): Unit = {
    ConfUtils.getString("jdbc.use")

    print("haha")
  }
}
