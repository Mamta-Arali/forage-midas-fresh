package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;

@Component
public class DatabaseConduit {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public DatabaseConduit(UserRepository userRepository,
                           TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    // Used by the tests to populate users
    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    // Find a user by ID
    public UserRecord findUser(long id) {
        return userRepository.findById(id);
    }

    // Save updated user balance
    public void saveUser(UserRecord user) {
        userRepository.save(user);
    }

    // Save a transaction record
    public void saveTransaction(TransactionRecord transaction) {
        transactionRepository.save(transaction);
    }
}
