package http

func SetAccountEmails(cfg *ServerConfig, emails map[string]string) {
	if cfg == nil || len(emails) == 0 {
		return
	}
	if cfg.AccountEmails == nil {
		cfg.AccountEmails = make(map[string]string, len(emails))
	}
	for username, email := range emails {
		cfg.AccountEmails[username] = email
	}
}

func GetAccountEmail(cfg *ServerConfig, username string) string {
	if cfg != nil {
		if email, ok := cfg.AccountEmails[username]; ok && email != "" {
			return email
		}
	}
	return username
}

func CleanupAccountEmails(_ *ServerConfig) {}
