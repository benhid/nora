build:
	@./gradlew shadowJar
	@cp build/libs/shadow.jar nora.jar

docs:
    @./gradlew javadoc

tests:
	@./gradlew clean test

.PHONY: build docs tests
