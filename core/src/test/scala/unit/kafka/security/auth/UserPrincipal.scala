package unit.kafka.security.auth

import java.io.Serializable
import java.security.Principal

class UserPrincipal(name : String) extends Principal with Serializable {

  def getName = {
    name;
  }
}