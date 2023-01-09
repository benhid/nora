build:
	@./gradlew shadowJar
	@cp build/libs/shadow.jar nora.jar

tests:
	@./gradlew clean test

.PHONY: build tests