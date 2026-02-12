package com.sepavliuk.capstone.config

case class Config(phoenix: Phoenix)
case class Phoenix(localRun: Boolean, jobName: String)
