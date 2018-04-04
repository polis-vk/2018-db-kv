package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.sudzhaev.KVDaoImpl;

public final class KVDaoFactory {
    private KVDaoFactory() {
        // Not instantiatable
    }

    @NotNull
    public static KVDao create() {
        return new KVDaoImpl();
    }
}
