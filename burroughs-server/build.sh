./mvnw clean package
cd client/burroughs-client
npm run build
cd ../..
docker image build -t burroughs_server .

