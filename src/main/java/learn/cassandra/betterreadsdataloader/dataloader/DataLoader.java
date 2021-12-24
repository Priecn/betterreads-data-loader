package learn.cassandra.betterreadsdataloader.dataloader;

import learn.cassandra.betterreadsdataloader.author.Author;
import learn.cassandra.betterreadsdataloader.author.AuthorRepository;
import learn.cassandra.betterreadsdataloader.book.Book;
import learn.cassandra.betterreadsdataloader.book.BookRepository;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
public class DataLoader {

    private final AuthorRepository authorRepository;
    private final BookRepository bookRepository;

    public DataLoader(AuthorRepository authorRepository, BookRepository bookRepository) {
        this.authorRepository = authorRepository;
        this.bookRepository = bookRepository;
    }


    @PostConstruct
    public void start() {
        System.out.println("Application started!! Initializing data");
        initAuthors();
        initWorks();
    }

    private void initAuthors() {

        try (Stream<String> lines = Files.lines(Path.of("src/main/resources/authors.txt"))) {
            lines.forEach(line -> {
                // read ans parse line
                int startPos = line.indexOf("{");
                String jsonString = line.substring(startPos);


                try {
                    JSONObject jsonObject = new JSONObject(jsonString);

                    // Construct Object

                    Author author = new Author();
                    String key = jsonObject.optString("key");
                    author.setId(key.replace("/authors/", ""));
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));

                    // persist to repo
                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }

            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initWorks() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try (Stream<String> lines = Files.lines(Path.of("src/main/resources/works.txt"))) {
            lines.forEach(line -> {
                // read ans parse line
                int startPos = line.indexOf("{");
                String jsonString = line.substring(startPos);


                try {
                    JSONObject jsonObject = new JSONObject(jsonString);

                    // Construct Object
                    Book book = new Book();
                    book.setId(jsonObject.getString("key").replace("/works/", ""));
                    book.setTitle(jsonObject.optString("title"));
                    JSONObject descriptionJson = jsonObject.optJSONObject("description");
                    if (descriptionJson != null) {
                        book.setDescription(descriptionJson.optString("value"));
                    }
                    JSONObject createdJson = jsonObject.optJSONObject("created");
                    if (createdJson != null) {
                        book.setPublishedDate(LocalDate.parse(createdJson.optString("value"), dateTimeFormatter));
                    }

                    JSONArray authorsJsonArr = jsonObject.optJSONArray("authors");

                    if (authorsJsonArr != null) {

                        List<String> authorIds = new ArrayList<>();


                        for (int i = 0; i < authorsJsonArr.length(); i++) {

                            JSONObject auhorJsonObj = authorsJsonArr.getJSONObject(i);
                            authorIds.add(auhorJsonObj.getJSONObject("author")
                                    .optString("key").replace("/authors/", ""));
                            book.setAuthorIds(authorIds);

                        }

                        List<String> authorNames = authorIds.stream().map(authorRepository::findById)
                                // .filter(authorOptional -> authorOptional.isPresent())
                                .map(authorOptional -> {
                                    if (authorOptional.isPresent())
                                        return authorOptional.get().getName();
                                    else
                                        return "Unknown Author";
                                })
                                .collect(Collectors.toList());

                        book.setAuthorNames(authorNames);
                    }



                    JSONArray conversJsonArr = jsonObject.optJSONArray("covers");
                    if(conversJsonArr != null) {
                        List<String> coverList = new ArrayList<>();

                        for (int i = 0; i < conversJsonArr.length(); i++) {
                            coverList.add(conversJsonArr.getString(i));
                        }

                        book.setCoverIds(coverList);
                    }

                    // persist to repo
                    bookRepository.save(book);

                } catch (JSONException e) {
                    e.printStackTrace();
                }

            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
