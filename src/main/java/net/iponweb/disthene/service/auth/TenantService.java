package net.iponweb.disthene.service.auth;

import java.util.Set;

/**
 * @author Andrei Ivanov
 */
public class TenantService {
    private Set<String> authorizedTenants;
    private boolean allowAll;

    public TenantService(Set<String> authorizedTenants, boolean allowAll) {
        this.authorizedTenants = authorizedTenants;
        this.allowAll = allowAll;
    }

    public boolean isTenantAllowed(String tenant) {
        return allowAll || authorizedTenants.contains(tenant);
    }

    public void setRules(Set<String> authorizedTenants, boolean allowAll) {
        this.authorizedTenants = authorizedTenants;
        this.allowAll = allowAll;
    }
}
