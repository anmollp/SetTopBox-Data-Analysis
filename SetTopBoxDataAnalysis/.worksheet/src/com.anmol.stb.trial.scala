package com.anmol.stb

import scala.xml.XML

object trial {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(436); 
  val line  = <d><nv n="ExtStationID" v="Station/FYI Television, Inc./25102" /><nv n="MediaDesc" v="19b8f4c0-92ce-44a7-a403-df4ee413aca9" /><nv n="ChannelNumber" v="1366" /><nv n="Duration" v="24375" /><nv n="IsTunedToService" v="True" /><nv n="StreamSelection" v="FULLSCREEN_PRIMARY" /><nv n="ChannelType" v="LiveTVMediaChannel" /><nv n="TuneID" v="636007629215440000" /></d>;System.out.println("""line  : scala.xml.Elem = """ + $show(line ));$skip(13); val res$0 = 
  line \ "d";System.out.println("""res0: scala.xml.NodeSeq = """ + $show(res$0))}
}
