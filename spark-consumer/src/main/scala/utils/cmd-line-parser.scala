package com.sparkmovie.utils

object CommandLineParser {

    def parseCmdLine(map : Map[String, String], args : List[String]) : Map[String, String] = {
        args match {
            case Nil => {
                map
            }
            case ("--brokers" | "-b") :: value :: tail => {
                parseCmdLine(map ++ Map("brokers" -> value), tail)
            }
            case ("--group-id" | "-gid") :: value :: tail => {
                parseCmdLine(map ++ Map("group-id" -> value), tail)
            }
            case ("--consume" | "-c") :: value :: tail => {
                parseCmdLine(map ++ Map("consume" -> value), tail)
            }
            case ("--produce" | "-p") :: value :: tail => {
                parseCmdLine(map ++ Map("produce" -> value), tail)
            }
            case ("--learning_data" | "-ld") :: value :: tail => {
                parseCmdLine(map ++ Map("learning_data" -> value), tail)
            }
            case string :: tail  => {
                parseCmdLine(map, tail)
            }
        }
    }

}
