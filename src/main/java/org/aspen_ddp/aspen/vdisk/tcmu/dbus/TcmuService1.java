package org.aspen_ddp.aspen.vdisk.tcmu.dbus;

import org.freedesktop.dbus.Tuple;
import org.freedesktop.dbus.annotations.DBusBoundProperty;
import org.freedesktop.dbus.annotations.DBusInterfaceName;
import org.freedesktop.dbus.annotations.DBusProperty;
import org.freedesktop.dbus.annotations.Position;
import org.freedesktop.dbus.interfaces.DBusInterface;
import org.freedesktop.dbus.types.UInt64;

/**
 * The D-Bus interface tcmu-runner exports for each handler (tcmu-handler.xml), which targetcli uses. Declarations
 * only: they are in Java because dbus-java derives D-Bus signatures from Java generic signatures, which Scala does
 * not emit for primitive type arguments. TcmuDbusService.scala implements it.
 */
@DBusInterfaceName("org.kernel.TCMUService1")
public interface TcmuService1 extends DBusInterface {

    /** Two out arguments. dbus-java 5.2.2's Introspect lists a generic Tuple's out args twice; the wire reply is right. */
    final class Reply<A, B> extends Tuple {
        @Position(0)
        public final A first;
        @Position(1)
        public final B second;

        public Reply(A first, B second) {
            this.first = first;
            this.second = second;
        }
    }

    /** CheckConfig(s configstr) -> (b is_valid, s message) */
    Reply<Boolean, String> CheckConfig(String configstr);

    /** ChangeMedium(s name, t size, s cfgstring) -> (i result, s message) */
    Reply<Integer, String> ChangeMedium(String name, UInt64 size, String cfgstring);

    @DBusBoundProperty(name = "ConfigDesc", access = DBusProperty.Access.READ)
    String getConfigDesc();
}
