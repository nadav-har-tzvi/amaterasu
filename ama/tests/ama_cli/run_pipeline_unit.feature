Feature: Run an amaterasu pipeline


  Scenario: Run a pipeline for a valid repository, should not raise an error

    Given A valid repository
    When Running a pipeline with the given repository
    Then An HandlerError should not be raised

  Scenario: Run a pipeline for a repository that doesn't exist, should raise an error
    Given A repository that doesn't exist
    When Running a pipeline with the given repository
    Then An HandlerError should be raised

  Scenario: Run a pipeline for a repository that is not amaterasu compliant, should raise an error
    Given A repository that is not Amaterasu compliant
    When Running a pipeline with the given repository
    Then An HandlerError should be raised

  Scenario: Run a pipeline for a valid repository by file URI should raise an error
    Given A valid file URI repository
    When Running a pipeline with the given repository
    Then An HandlerError should be raised