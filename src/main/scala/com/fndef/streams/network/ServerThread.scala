package com.fndef.streams.network

import com.fndef.streams.core.Startable

abstract class ServerThread(val id: String) extends Thread(id) with Startable
