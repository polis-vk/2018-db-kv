package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dimik2010.KVDaoHashMapImplementation;

import java.io.IOException;

public final class KVDaoFactory {
    private KVDaoFactory() {
        // Not instantiatable
    }

    @NotNull
    public static KVDao create() throws IOException {
        return new KVDaoHashMapImplementation();
    }
}
