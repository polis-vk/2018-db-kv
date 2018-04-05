package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.vana06.KVDaoImpl;

import java.io.IOException;

public final class KVDaoFactory {
    private KVDaoFactory() {
        // Not instantiatable
    }

    @NotNull
    public static KVDao create() throws IOException {
        //throw new UnsupportedOperationException("Implement me");
        return new KVDaoImpl();
    }
}
