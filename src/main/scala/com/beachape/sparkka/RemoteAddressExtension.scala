package com.beachape.sparkka

import akka.actor.{ ExtensionKey, Extension, ExtendedActorSystem }

/**
 * Created by Lloyd on 2/28/16.
 */

object RemoteAddressExtension extends ExtensionKey[RemoteAddressExtensionImpl]

class RemoteAddressExtensionImpl(system: ExtendedActorSystem) extends Extension {
  def address = system.provider.getDefaultAddress
}

