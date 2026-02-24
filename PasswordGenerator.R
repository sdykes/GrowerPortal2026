
password_lookup <- read_csv("userdb2026.csv", show_col_types = FALSE) |>
  mutate(Permissions = str_replace_all(Permissions, fixed(" "), "")) 

# Define character sets including lowercase, uppercase, digits, and symbols
char_sets <- c(letters, LETTERS, 0:9)

# Function to generate a single password of specified length
generate_password <- function(length = 10) {
  password_chars <- sample(char_sets, length, replace = TRUE)
  return(paste(password_chars, collapse = ""))
}

# Function to generate multiple passwords
generate_multiple_passwords <- function(n_passwords, length = 10) {
  passwords <- character(n_passwords)
  for (i in 1:n_passwords) {
    passwords[i] <- generate_password(length)
  }
  return(passwords)
}

generate_password()

# Example usage: Generate 5 passwords, each 12 characters long
my_passwords <- generate_multiple_passwords(n_passwords = nrow(password_lookup), length = 10)

pwList <- tibble(Password = my_passwords)

new_userdb <- password_lookup |>
  select(-Password) |>
  bind_cols(pwList)

write_csv(new_userdb, "userdb2026.csv")
