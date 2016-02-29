package com.beachape.sparkka

import akka.actor.{ ExtensionKey, Extension, ExtendedActorSystem }

/**
 * Created by Lloyd on 2/28/16.
 */

private[sparkka] object RemoteAddressExtension extends ExtensionKey[RemoteAddressExtensionImpl]

private[sparkka] class RemoteAddressExtensionImpl(system: ExtendedActorSystem) extends Extension {
  def address = system.provider.getDefaultAddress
}

