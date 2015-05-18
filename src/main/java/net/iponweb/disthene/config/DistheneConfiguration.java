package net.iponweb.disthene.config;

/**
 * @author Andrei Ivanov
 */
@SuppressWarnings("UnusedDeclaration")
public final class DistheneConfiguration {
    private CarbonConfiguration carbon;
    private StoreConfiguration store;


    public CarbonConfiguration getCarbon() {
        return carbon;
    }

    public void setCarbon(CarbonConfiguration carbon) {
        this.carbon = carbon;
    }

    public StoreConfiguration getStore() {
        return store;
    }

    public void setStore(StoreConfiguration store) {
        this.store = store;
    }

    @Override
    public String toString() {
        return "DistheneConfiguration{" +
                "carbon=" + carbon +
                ", store=" + store +
                '}';
    }
}
